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

import io.questdb.PropertyKey;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.griffin.SqlException;
import org.junit.Test;

/**
 * Live views on a materialized view across the view's own DDL. Live-view invalidation must not change (design,
 * "Behaviour of base and parent operations"): a view DROP COLUMN invalidates only the live views that read the
 * column, as a base DROP COLUMN does; ADD COLUMN, DROP PARTITION, ALTER ... AS and RESUME leave the live views
 * active and feeding on the view's commits; a FULL refresh of the view invalidates them as a rebuild.
 * <p>
 * The live views sum a column over the current and the three preceding buckets of a key, so expected rows are
 * running sums of the view's buckets.
 */
public class LiveViewMatViewDdlTest extends AbstractLiveViewTest {

    @Test
    public void testAddColumnAndDropPartitionKeepLiveViewActive() throws Exception {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            createViewAndLiveViews();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertTwoBuckets(job);

                execute("ALTER MATERIALIZED VIEW mvbase ADD COLUMN extra INT");
                drainWalAndMatViewQueues();
                driveRefreshToQuiescence(job);
                execute("INSERT INTO base VALUES ('2026-01-01T02:00:00.000000Z', 'a', 3.0, 30.0)");
                drainWalAndMatViewQueues();
                driveRefreshToQuiescence(job);
                assertLiveViewStatus("active", "", "active", "");
                assertQuery("lv_v ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("""
                        ts\tk\ts
                        2026-01-01T00:00:00.000000Z\ta\t1.0
                        2026-01-01T01:00:00.000000Z\ta\t3.0
                        2026-01-01T02:00:00.000000Z\ta\t6.0
                        """);

                // A view DROP PARTITION, like a TTL drop, invalidates no live view.
                execute("INSERT INTO base VALUES ('2026-01-02T00:00:00.000000Z', 'a', 4.0, 40.0)");
                drainWalAndMatViewQueues();
                driveRefreshToQuiescence(job);
                execute("ALTER MATERIALIZED VIEW mvbase DROP PARTITION LIST '2026-01-01'");
                drainWalAndMatViewQueues();
                driveRefreshToQuiescence(job);
                assertLiveViewStatus("active", "", "active", "");
                assertNoRefreshFaults("lv_v");
                assertNoRefreshFaults("lv_w");
            }
        });
    }

    @Test
    public void testAlterAsKeepsLiveViewActiveAndFullInvalidatesIt() throws Exception {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            createViewAndLiveViews();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertTwoBuckets(job);

                // The new SQL doubles sv for the buckets the view refreshes from now on.
                execute("ALTER MATERIALIZED VIEW mvbase AS (SELECT ts, k, sum(v) * 2 AS sv, sum(w) AS sw FROM base SAMPLE BY 1h)");
                drainWalAndMatViewQueues();
                driveRefreshToQuiescence(job);
                execute("INSERT INTO base VALUES ('2026-01-01T02:00:00.000000Z', 'a', 3.0, 30.0)");
                drainWalAndMatViewQueues();
                driveRefreshToQuiescence(job);
                assertLiveViewStatus("active", "", "active", "");
                assertQuery("lv_v ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("""
                        ts\tk\ts
                        2026-01-01T00:00:00.000000Z\ta\t1.0
                        2026-01-01T01:00:00.000000Z\ta\t3.0
                        2026-01-01T02:00:00.000000Z\ta\t9.0
                        """);

                execute("REFRESH MATERIALIZED VIEW mvbase FULL");
                drainWalAndMatViewQueues();
                driveRefreshToQuiescence(job);
                assertLiveViewStatus("invalid", "base materialized view was rebuilt", "invalid", "base materialized view was rebuilt");
            }
            drainWalQueue();
            // FULL rebuilt every bucket with the new SQL
            assertQuery("mvbase ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("""
                    ts\tk\tsv\tsw
                    2026-01-01T00:00:00.000000Z\ta\t2.0\t10.0
                    2026-01-01T01:00:00.000000Z\ta\t4.0\t20.0
                    2026-01-01T02:00:00.000000Z\ta\t6.0\t30.0
                    """);
        });
    }

    @Test
    public void testDropColumnInvalidatesOnlyLiveViewReadingIt() throws Exception {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            createViewAndLiveViews();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertTwoBuckets(job);
                assertQuery("lv_w ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("""
                        ts\tk\ts
                        2026-01-01T00:00:00.000000Z\ta\t10.0
                        2026-01-01T01:00:00.000000Z\ta\t30.0
                        """);

                execute("ALTER MATERIALIZED VIEW mvbase DROP COLUMN sw");
                drainWalAndMatViewQueues();
                driveRefreshToQuiescence(job);
                assertLiveViewStatus("active", "", "invalid", "drop column operation [column=sw]");

                execute("INSERT INTO base VALUES ('2026-01-01T02:00:00.000000Z', 'a', 3.0, 30.0)");
                drainWalAndMatViewQueues();
                driveRefreshToQuiescence(job);
                assertLiveViewStatus("active", "", "invalid", "drop column operation [column=sw]");
                assertQuery("lv_v ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("""
                        ts\tk\ts
                        2026-01-01T00:00:00.000000Z\ta\t1.0
                        2026-01-01T01:00:00.000000Z\ta\t3.0
                        2026-01-01T02:00:00.000000Z\ta\t6.0
                        """);
                assertNoRefreshFaults("lv_v");
            }
        });
    }

    @Test
    public void testResumeCatchUpFeedsLiveView() throws Exception {
        // The view is invalid while the base receives a row; RESUME catches it up, and the live view consumes the
        // catch-up refresh's commit like any other.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            createViewAndLiveViews();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertTwoBuckets(job);

                engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("mvbase"), "test invalidation");
                drainWalAndMatViewQueues();
                execute("INSERT INTO base VALUES ('2026-01-01T02:00:00.000000Z', 'a', 3.0, 30.0)");
                drainWalAndMatViewQueues();
                driveRefreshToQuiescence(job);
                // the invalid view did not refresh, so the live view has nothing new
                assertQuery("lv_v ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("""
                        ts\tk\ts
                        2026-01-01T00:00:00.000000Z\ta\t1.0
                        2026-01-01T01:00:00.000000Z\ta\t3.0
                        """);

                execute("REFRESH MATERIALIZED VIEW mvbase RESUME");
                drainWalAndMatViewQueues();
                driveRefreshToQuiescence(job);
                assertLiveViewStatus("active", "", "active", "");
                assertQuery("lv_v ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("""
                        ts\tk\ts
                        2026-01-01T00:00:00.000000Z\ta\t1.0
                        2026-01-01T01:00:00.000000Z\ta\t3.0
                        2026-01-01T02:00:00.000000Z\ta\t6.0
                        """);
                assertNoRefreshFaults("lv_v");
            }
        });
    }

    private void assertLiveViewStatus(String vStatus, String vReason, String wStatus, String wReason) throws Exception {
        assertQuery("SELECT view_name, view_status, invalidation_reason FROM live_views() ORDER BY view_name")
                .noLeakCheck()
                .returns("view_name\tview_status\tinvalidation_reason\n"
                        + "lv_v\t" + vStatus + '\t' + vReason + '\n'
                        + "lv_w\t" + wStatus + '\t' + wReason + '\n');
    }

    // A view of hourly sums of v and w, and two live views: lv_v over sv, lv_w over sw. Both start before any row,
    // so they consume the view's commits incrementally.
    private static void createViewAndLiveViews() throws SqlException {
        execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE, w DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE MATERIALIZED VIEW mvbase AS (SELECT ts, k, sum(v) AS sv, sum(w) AS sw FROM base SAMPLE BY 1h) PARTITION BY DAY");
        drainWalAndMatViewQueues();
        setCurrentMicros(0L);
        execute("CREATE LIVE VIEW lv_v FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, k, sum(sv) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM mvbase");
        execute("CREATE LIVE VIEW lv_w FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, k, sum(sw) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM mvbase");
    }

    // Buckets a@00:00 (v 1, w 10) and a@01:00 (v 2, w 20).
    private void insertTwoBuckets(LiveViewRefreshJob job) throws Exception {
        execute("""
                INSERT INTO base VALUES
                ('2026-01-01T00:00:00.000000Z', 'a', 1.0, 10.0),
                ('2026-01-01T01:00:00.000000Z', 'a', 2.0, 20.0)
                """);
        drainWalAndMatViewQueues();
        driveRefreshToQuiescence(job);
        assertQuery("lv_v ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("""
                ts\tk\ts
                2026-01-01T00:00:00.000000Z\ta\t1.0
                2026-01-01T01:00:00.000000Z\ta\t3.0
                """);
    }
}
