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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * A live view with no refresh worker must be inert, not half-alive.
 * <p>
 * {@code live.view.refresh.worker.count=0} starts no {@code LiveViewRefreshJob}, so nothing
 * advances a view's {@code lvConsumedSeqTxn}. Registering the view anyway made
 * {@code WalPurgeJob} clamp the base WAL purge floor to that frozen watermark forever, and the
 * base WAL plus the sequencer txn-log parts then grew until the disk filled. {@code CREATE LIVE
 * VIEW} also succeeded and handed back a view that would never seed, never drain and never
 * serve a row.
 * <p>
 * Both are gated on {@code CairoConfiguration.isLiveViewRefreshEnabled()} now, so a zero worker
 * count behaves exactly like {@code cairo.live.view.enabled=false}: no registration, no
 * retention, no CREATE. Views already on disk stay queryable as plain tables.
 */
public class LiveViewRefreshDisabledTest extends AbstractBootstrapTest {

    private static final String CREATE_LIVE_VIEW = "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
            "SELECT val, ts, row_number() OVER w AS rn FROM base " +
            "WINDOW w AS (PARTITION BY val ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))";

    @Before
    @Override
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
    }

    @Test
    public void testCreateLiveViewIsRejectedWithoutARefreshWorker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = start("0")) {
                serverMain.execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
                try {
                    serverMain.getEngine().execute(CREATE_LIVE_VIEW, serverMain.getSqlExecutionContext());
                    Assert.fail("CREATE LIVE VIEW must not succeed without a refresh worker");
                } catch (SqlException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "live view refresh is disabled, set live.view.refresh.worker.count to a positive value"
                    );
                }
                // The feature flag is still on, so the parser reached the worker-count gate
                // rather than the "live views are disabled" one above it.
                serverMain.assertSql("SELECT count() FROM live_views()", "count\n0\n");
            }
        });
    }

    @Test
    public void testUnattendedLiveViewIsNotRegisteredAndReleasesTheBaseWalFloor() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = start("1")) {
                serverMain.execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
                serverMain.execute("INSERT INTO base VALUES " +
                        "(1, '2024-01-01T00:00:00.000000Z')," +
                        "(2, '2024-01-01T00:00:01.000000Z')," +
                        "(3, '2024-01-01T00:00:02.000000Z')");
                TestUtils.assertEventually(
                        () -> serverMain.assertSql("SELECT count(*) FROM base", "count\n3\n"),
                        30
                );
                serverMain.execute(CREATE_LIVE_VIEW);
                TestUtils.assertEventually(
                        () -> serverMain.assertSql("SELECT count(*) FROM lv", "count\n3\n"),
                        60
                );
                // The view holds a real floor while a worker attends it - that is what the
                // restart below has to release.
                serverMain.assertSql("SELECT count() FROM live_views()", "count\n1\n");
            }

            // Same data directory, refresh pool switched off.
            try (final TestServerMain serverMain = start("0")) {
                serverMain.assertSql("SELECT count() FROM live_views()", "count\n0\n");
                // Unregistered is not unavailable: the read path falls back to the plain disk
                // cursor, so the view still serves everything the last refresh materialised.
                serverMain.assertSql("SELECT count(*) FROM lv", "count\n3\n");

                final String baseDirName = serverMain.getEngine().verifyTableName("base").getDirName();
                for (int i = 0; i < 40; i++) {
                    serverMain.execute("INSERT INTO base VALUES (" + (100 + i) +
                            ", '2024-01-01T01:00:00.000000Z')");
                }
                TestUtils.assertEventually(
                        () -> serverMain.assertSql("SELECT count(*) FROM base", "count\n43\n"),
                        60
                );

                // One segment per commit (rollover row count 1), so the retained count reads the
                // purge floor directly. With the view's frozen watermark still clamping
                // safeToPurgeTxn every segment above it survives, and the count tracks the 40
                // commits instead of settling. Pre-fix this poll never converges.
                TestUtils.assertEventually(
                        () -> {
                            final int retained = countWalSegments(baseDirName);
                            Assert.assertTrue(
                                    "base WAL must not be retained for an unattended live view, retained=" + retained,
                                    retained < 10
                            );
                        },
                        60
                );

                // A view that cannot be refreshed cannot be created either, even though one is
                // already on disk.
                try {
                    serverMain.getEngine().execute(
                            "CREATE LIVE VIEW lv2 FLUSH EVERY 100ms START FROM BEGINNING AS " +
                                    "SELECT val, ts, row_number() OVER w AS rn FROM base " +
                                    "WINDOW w AS (PARTITION BY val ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))",
                            serverMain.getSqlExecutionContext()
                    );
                    Assert.fail("CREATE LIVE VIEW must not succeed without a refresh worker");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "live view refresh is disabled");
                }
            }
        });
    }

    /**
     * Counts the base table's live WAL segment directories - {@code db/<tableDir>/walN/<segment>}.
     * WalPurgeJob removes them from the bottom up as the purge floor advances, so the count is a
     * direct read of how much base WAL is still retained.
     */
    private static int countWalSegments(String tableDirName) {
        final File tableDir = new File(root + File.separator + "db" + File.separator + tableDirName);
        final File[] walDirs = tableDir.listFiles();
        if (walDirs == null) {
            return 0;
        }
        int count = 0;
        for (File walDir : walDirs) {
            if (!walDir.isDirectory() || !walDir.getName().startsWith("wal")) {
                continue;
            }
            final File[] segments = walDir.listFiles();
            if (segments == null) {
                continue;
            }
            for (File segment : segments) {
                if (segment.isDirectory()) {
                    count++;
                }
            }
        }
        return count;
    }

    private static TestServerMain start(String liveViewRefreshWorkerCount) {
        return startWithEnvVariables(
                PropertyKey.CAIRO_LIVE_VIEW_ENABLED.getEnvVarName(), "true",
                PropertyKey.LIVE_VIEW_REFRESH_WORKER_COUNT.getEnvVarName(), liveViewRefreshWorkerCount,
                // One WAL segment per commit, so countWalSegments reads the purge floor rather
                // than the rollover threshold.
                PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT.getEnvVarName(), "1",
                PropertyKey.CAIRO_WAL_PURGE_INTERVAL.getEnvVarName(), "10",
                PropertyKey.HTTP_MIN_ENABLED.getEnvVarName(), "false",
                PropertyKey.PG_ENABLED.getEnvVarName(), "false"
        );
    }
}
