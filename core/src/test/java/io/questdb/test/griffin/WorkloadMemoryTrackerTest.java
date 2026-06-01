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
import org.junit.BeforeClass;
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
 * allocator -- writer/O3 memory is out of scope by design. So the WAL_APPLY
 * tests verify the acquire/bind/release lifecycle is correct and balanced under
 * an active limit rather than forcing a breach; the tracker is wired so any
 * future SQL-reachable allocation charges WAL_APPLY rather than the QUERY budget.
 * <p>
 * The limits are set in {@link #beforeClass()} because
 * {@code CairoEngine#getMemoryTrackerProvider} caches the provider on first
 * access. The QUERY limit is left unlimited so the test's own setup and
 * assertion queries are never constrained -- only the two background workloads
 * are.
 */
public class WorkloadMemoryTrackerTest extends AbstractCairoTest {

    @BeforeClass
    public static void beforeClass() {
        // 512 KiB clears the small-workload initial allocations (group-by maps) for
        // the success cases, while the large-workload breach case (tens of thousands
        // of distinct keys) exceeds it after a few heap doublings.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 512 * 1024L);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_MEMORY_LIMIT_BYTES, 512 * 1024L);
        // Drop the per-retry backoff sleep: a per-query breach is deterministic
        // across the refresh's step-reduction retries, so the sleeps only slow the
        // test down before the refresh gives up and invalidates the view.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_OOM_RETRY_TIMEOUT, 0);
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
            assertQueryNoLeakCheck(
                    "view_status\toom\tworkload\n" +
                            "invalid\ttrue\ttrue\n",
                    "SELECT view_status, " +
                            "invalidation_reason LIKE '%query memory limit exceeded%' AS oom, " +
                            "invalidation_reason LIKE '%workload=MAT_VIEW_REFRESH%' AS workload " +
                            "FROM materialized_views WHERE view_name = 'mv'",
                    null
            );
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
            assertQueryNoLeakCheck(
                    "view_status\n" +
                            "valid\n",
                    "SELECT view_status FROM materialized_views WHERE view_name = 'mv'",
                    null
            );
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

            assertQueryNoLeakCheck(
                    "view_status\n" +
                            "valid\n",
                    "SELECT view_status FROM materialized_views WHERE view_name = 'mv'",
                    null
            );
        });
    }

    @Test
    public void testWalApplyUpdateRepeatedRunsReleaseAllocations() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT x::int, x::long, timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(10)");
            drainWalQueue();

            // Each apply acquires and releases a WAL_APPLY tracker; assertMemoryLeak
            // (plus the live recordPerQueryMemAlloc assert) catches an unbalanced
            // acquire/release or a tracker leak across the repeated batches.
            for (int i = 0; i < 5; i++) {
                execute("UPDATE t SET v = v + 1 WHERE k > 5");
                drainWalQueue();
            }
            assertQueryNoLeakCheck(
                    "suspended\n" +
                            "false\n",
                    "SELECT suspended FROM wal_tables() WHERE name = 't'",
                    null
            );
        });
    }

    @Test
    public void testWalApplyUpdateSucceedsUnderActiveLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT x::int, x::long, timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(10)");
            drainWalQueue();

            // A join- and subquery-free UPDATE is the only shape WAL apply runs; it
            // reaches no tracker-wired allocator, so it applies cleanly under the
            // active WAL_APPLY limit. This guards the acquire/release wiring against
            // breaking normal WAL apply.
            execute("UPDATE t SET v = v + 100 WHERE k > 5");
            drainWalQueue();

            assertQueryNoLeakCheck(
                    "suspended\n" +
                            "false\n",
                    "SELECT suspended FROM wal_tables() WHERE name = 't'",
                    null
            );
            // v = 1..10; rows k = 6..10 each gain 100, so sum = 55 + 5 * 100 = 555.
            // The non-keyed aggregate cursor reports a fixed size of 1, hence the
            // explicit expectSize flag.
            assertQueryNoLeakCheck(
                    "sum\n" +
                            "555\n",
                    "SELECT sum(v) AS sum FROM t",
                    null,
                    null,
                    false,
                    true
            );
        });
    }
}
