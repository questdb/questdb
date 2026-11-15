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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;

/**
 * Live views over the PostgreSQL wire protocol.
 */
public class PGLiveViewTest extends BasePGTest {
    private static final String VIEW_SELECT = "SELECT ts, x, count(*) OVER (PARTITION BY 0 ORDER BY ts "
            + "ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE x > 0";

    @Test
    public void testAlterLiveViewWalControlAsPreparedStatementsExecutedTwice() throws Exception {
        // ALTER LIVE VIEW ... SUSPEND WAL and ALTER LIVE VIEW ... RESUME WAL as prepared statements, each
        // executed twice. Neither repeat fails: the first pair pauses the view, which then serves the rows it
        // had while the base moves on, and the second pair resumes it, after which it holds every row.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " + VIEW_SELECT);
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2026-04-01T00:00:00.000000Z', 1)");
                drive(job);
                try (PreparedStatement suspend = connection.prepareStatement("ALTER LIVE VIEW lv SUSPEND WAL WITH 'DISK FULL', 'manual halt'")) {
                    suspend.execute();
                    suspend.execute();
                }
                Assert.assertTrue(engine.isWalApplySuspended(lvToken));
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(lvToken));
                execute("INSERT INTO base VALUES ('2026-04-01T01:00:00.000000Z', 2)");
                drive(job);
                assertViewRows("ts\tx\trn\n2026-04-01T00:00:00.000000Z\t1\t1\n");

                try (PreparedStatement resume = connection.prepareStatement("ALTER LIVE VIEW lv RESUME WAL")) {
                    resume.execute();
                    resume.execute();
                }
                Assert.assertFalse(engine.isWalApplySuspended(lvToken));
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(lvToken));
                drive(job);
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, VIEW_SELECT, "lv", LOG);
                assertViewRows("ts\tx\trn\n"
                        + "2026-04-01T00:00:00.000000Z\t1\t1\n"
                        + "2026-04-01T01:00:00.000000Z\t2\t2\n");
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    // Applies the base and runs the refresh job with the clock past FLUSH EVERY until it reports no work.
    private static void drive(LiveViewRefreshJob job) {
        for (int pass = 0; pass < 64; pass++) {
            setCurrentMicros(currentMicros + 2_000_000L);
            drainWalQueue();
            boolean isWork = false;
            for (int i = 0; i < 64 && job.run(); i++) {
                isWork = true;
            }
            drainWalQueue();
            if (!isWork) {
                return;
            }
        }
        Assert.fail("the refresh job did not quiesce");
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("SELECT ts, x, rn FROM lv")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(expected);
    }
}
