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

import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

/**
 * The tx table and the live view lv over it - a running sum and count per account, reset each day by
 * an ANCHOR DAILY window - that the tests of a live view's refresh faults, pauses and recoveries
 * drive, with the plain-SQL recompute they check the view against.
 */
abstract class AbstractLiveViewTxTest extends AbstractLiveViewTest {
    static final String RECOMPUTE_SQL = """
            (
                SELECT created_at, account_id,
                    sum(amount) OVER (PARTITION BY account_id, bucket ORDER BY created_at
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum,
                    count(account_id) OVER (PARTITION BY account_id, bucket ORDER BY created_at
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_count
                FROM (
                    SELECT created_at, account_id, amount,
                        timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp) AS bucket
                    FROM tx
                )
            ) ORDER BY 2, 1
            """;
    static final String VIEW_SQL = "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum, "
            + "count(account_id) OVER w AS cumulative_count "
            + "FROM tx WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')";

    static void assertViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(engine, sqlExecutionContext, RECOMPUTE_SQL, "(lv) ORDER BY 2, 1", LOG, true);
    }

    static void insert(String values) throws SqlException {
        execute("INSERT INTO tx (created_at, account_id, amount) VALUES " + values);
        drainWalQueue();
    }

    static LiveViewInstance instance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    static void removeTimeline() {
        try (
                Path dir = new Path().of(configuration.getDbRoot())
                        .concat(instance().getLiveViewToken())
                        .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
                Path timelinePath = new Path()
        ) {
            LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
            Assert.assertTrue(
                    "the fixture must have published a timeline to remove",
                    configuration.getFilesFacade().removeQuiet(timelinePath.$())
            );
        }
    }

    // What a process stop does: the registry drops its instances and every reader and writer goes back.
    static void shutdown() {
        engine.getLiveViewRegistry().clear();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactive();
    }

    // tx (created_at, account_id, amount) with four commits on 2026-01-01, each refreshed into lv.
    void createFixture(String dedupClause) throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY DAY WAL " + dedupClause);
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + VIEW_SQL);
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            final String[] commits = {
                    "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
                    "('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0)",
                    "('2026-01-01T09:20:00.000000Z', 'acct-1', 4.0)",
                    "('2026-01-01T09:30:00.000000Z', 'acct-2', 8.0)"
            };
            for (String values : commits) {
                insert(values);
                driveRefreshToQuiescence(job);
            }
        }
        assertViewMatchesRecompute();
        assertNoRefreshFaults("lv");
    }

    // A deduplicating base and the view over it at the default seal cadence, which seals the first boundary
    // only, with a last commit whose rows are out of order within it and collide on the dedup keys. The
    // collision routes the drain through the applied base, whose reader yields them in order, so no repair
    // runs and no root covers the commit; the raw WAL a restore replays still holds them unsorted.
    void createO3GapFixture() throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY DAY WAL DEDUP UPSERT KEYS(created_at, account_id)");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + VIEW_SQL);
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            final String[] commits = {
                    "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
                    "('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0)",
                    "('2026-01-03T09:00:00.000000Z', 'acct-1', 8.0), ('2026-01-03T09:10:00.000000Z', 'acct-1', 16.0), "
                            + "('2026-01-03T09:20:00.000000Z', 'acct-1', 32.0)",
                    "('2026-01-03T09:50:00.000000Z', 'acct-1', 64.0), ('2026-01-03T09:50:00.000000Z', 'acct-1', 65.0), "
                            + "('2026-01-03T09:40:00.000000Z', 'acct-1', 128.0)"
            };
            for (String values : commits) {
                insert(values);
                driveRefreshToQuiescence(job);
            }
        }
        assertViewMatchesRecompute();
        assertNoRefreshFaults("lv");
    }
}
