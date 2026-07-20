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
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * {@code ServerMain} wires live-view refresh under its own enable flag, not the
 * mat-view flag.
 * <p>
 * {@code setupDedicatedPools} shares one dedicated pool between mat-view and
 * live-view refresh. Gating the live-view job on {@code cairo.mat.view.enabled}
 * would silently drop refresh when mat views are off and live views on (the
 * default): {@code CREATE LIVE VIEW} succeeds and the state store enqueues refresh
 * tasks, but nothing ever drains them. This test runs a server with mat views
 * disabled + live views enabled and asserts the view still refreshes.
 */
public class LiveViewMatViewDisabledTest extends AbstractBootstrapTest {

    @Before
    @Override
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
    }

    @Test
    public void testLiveViewRefreshesWithMatViewsDisabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "false",
                    PropertyKey.CAIRO_LIVE_VIEW_ENABLED.getEnvVarName(), "true",
                    // Guarantee the shared refresh pool exists regardless of the host's
                    // CPU-derived default worker count.
                    PropertyKey.MAT_VIEW_REFRESH_WORKER_COUNT.getEnvVarName(), "1",
                    PropertyKey.HTTP_MIN_ENABLED.getEnvVarName(), "false",
                    PropertyKey.PG_ENABLED.getEnvVarName(), "false"
            )) {
                serverMain.execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
                serverMain.execute("INSERT INTO base VALUES " +
                        "(1, '2024-01-01T00:00:00.000000Z')," +
                        "(2, '2024-01-01T00:00:01.000000Z')," +
                        "(3, '2024-01-01T00:00:02.000000Z')");
                // Wait for the base rows to apply before creating the SEED view, so
                // the sweep pins a snapshot that already holds all three rows.
                TestUtils.assertEventually(
                        () -> serverMain.assertSql("SELECT count(*) FROM base", "count\n3\n"),
                        30
                );

                // SEED admits the pre-existing (historical-timestamp) rows regardless
                // of the view's real-time lower bound, so the assertion is deterministic.
                serverMain.execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                        "SELECT val, ts, row_number() OVER w AS rn FROM base " +
                        "WINDOW w AS (PARTITION BY val ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

                // With the refresh job wired, the seed sweep materialises all three
                // base rows. Pre-fix (live-view refresh gated on the mat-view flag)
                // nothing drives the sweep and this poll never converges.
                TestUtils.assertEventually(
                        () -> serverMain.assertSql("SELECT count(*) FROM lv", "count\n3\n"),
                        60
                );
            }
        });
    }
}
