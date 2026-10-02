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
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A restart whose refresh worker reaches the metadata cache before the startup hydrator has
 * put the view's base table in it.
 * <p>
 * {@code MetadataCache.onStartupAsyncHydrator} runs on its own thread and walks the catalogue
 * one table at a time under a write lock per table, so on an instance with many tables there
 * is a real window where a live view's base table is registered, readable by every SQL cursor
 * in the process, and simply not in the cache yet. {@code AbstractPartitionFrameCursorFactory}
 * - the generic SQL read path - closes that window by calling
 * {@code hydrateTableOnDemand(token)} before it reads. {@link LiveViewRefreshJob} did not.
 * <p>
 * The cost was not a wrong answer but a silently discarded one. The restore is the first thing
 * a restart runs; it resolves the base projection through {@code buildColumnMappings}, which
 * read the cache cold, found no entry and threw {@code table does not exist}.
 * {@code tryRestoreFromTimeline}'s {@code catch (Throwable)} cannot tell that apart from an
 * unreadable timeline, so it did what an unreadable timeline deserves: retire the checkpoint
 * ladder and recompute the whole window from the base table. The view stayed valid and its rows
 * stayed correct, which is why nothing caught it - the only visible trace was a checkpoint
 * ladder that reset to a single boundary on a restart that should have resumed at the head,
 * and a full replay of the base on every such restart.
 * <p>
 * {@code clearCache()} with no {@code hydrateAllTables()} after it is exactly that startup
 * state: an empty {@code tableMap} with {@code cacheComplete} unset. The case asserts the cache
 * really is cold before it drives a refresh, because a harness that quietly warmed it would
 * turn this into a test of nothing.
 */
public class LiveViewColdMetadataCacheTest extends AbstractLiveViewCheckpointCompatTest {

    private static final int BOUNDARIES = 5;
    private static final String DAILY_ANCHOR = "2026-01-01T";
    private static final String HEAD_BOUNDARY = "2026-01-01T09:00:40.000000Z";
    private static final LogCapture capture = new LogCapture();

    @After
    public void resetClock() {
        capture.stop();
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical boundary per commit, so a retired ladder is visibly shorter than a
        // resumed one rather than a difference of zero.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
        capture.start();
    }

    @Test
    public void testARestartAheadOfTheHydratorStillRestoresOffItsCheckpointLadder() throws Exception {
        assertMemoryLeak(() -> {
            seedFiveBoundaries();
            final TableToken baseToken = instance("lv").getDefinition().getBaseTableToken();

            // Re-arm the capture so the two assertNotLogged below describe the restart alone.
            // Left spanning the seed, they would be asserting something about the seed as well,
            // and would fail for the wrong reason the day a seed legitimately rebuilds.
            capture.start();

            restartWithAColdCatalogue();
            Assert.assertNull(
                    "the case is only meaningful while the catalogue is cold; something warmed it",
                    baseTableInCache(baseToken)
            );

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse("a cold catalogue must not invalidate the view", instance.isInvalid());
            Assert.assertTrue("the restore must have run", instance.isCheckpointRestoreAttempted());
            assertNoRefreshFaults("lv");

            // The load-bearing pair. isCheckpointRestoreSucceeded() cannot carry this on its
            // own - the from-base rebuild sets it too - so the witness is the ladder the
            // rebuild would have retired, and the absence of the rebuild's own log line.
            capture.drain();
            capture.assertNotLogged("could not restore live view from checkpoint timeline, rebuilding derived state");
            capture.assertNotLogged("live view restart rebuilding from applied base");
            Assert.assertEquals(
                    "a restart ahead of the hydrator must resume the ladder, not retire it",
                    BOUNDARIES,
                    countSealedBoundaries("lv")
            );
            Assert.assertEquals(
                    "the restored runtime must resume at the boundary the last seal published",
                    ts(HEAD_BOUNDARY),
                    instance.getHeadCheckpointMaxTs()
            );

            // A resume that read a half-built projection would be worse than one that failed,
            // so the rows are compared against a from-base recompute as well.
            assertViewMatchesRecompute();

            // The resumed runtime keeps accumulating: a projection resolved off a cold cache
            // would answer this row's own amount rather than the running total.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO tx VALUES ('" + timestamp(50) + "', 'acct-1', 100.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
            assertNoRefreshFaults("lv");
            assertQuery("SELECT created_at, account_id, cumulative_sum FROM lv")
                    .timestamp("created_at")
                    .expectSize()
                    .returns("created_at\taccount_id\tcumulative_sum\n" +
                            "2026-01-01T09:00:00.000000Z\tacct-1\t1.0\n" +
                            "2026-01-01T09:00:10.000000Z\tacct-2\t11.0\n" +
                            "2026-01-01T09:00:20.000000Z\tacct-1\t22.0\n" +
                            "2026-01-01T09:00:30.000000Z\tacct-2\t42.0\n" +
                            "2026-01-01T09:00:40.000000Z\tacct-1\t63.0\n" +
                            "2026-01-01T09:00:50.000000Z\tacct-1\t163.0\n");
        });
    }

    private static String timestamp(int secondOfDay) {
        return DAILY_ANCHOR + String.format("09:%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
    }

    /**
     * Compares the view against a from-base recompute of the same window. ANCHOR is live-view
     * syntax, so the daily bucket is written out as an ordinary partition term.
     */
    private void assertViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(select created_at, account_id, "
                        + "sum(amount) over (partition by account_id, bucket order by created_at "
                        + "rows between unbounded preceding and current row) as cumulative_sum "
                        + "from (select created_at, account_id, amount, " + bucket + " as bucket from tx)"
                        + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
    }

    /**
     * Whether the catalogue currently holds the base table, read without hydrating it. Any
     * lookup that hydrates on demand would answer its own question.
     */
    private Object baseTableInCache(TableToken baseToken) {
        try (MetadataCacheReader metaRO = engine.getMetadataCache().readLock()) {
            return metaRO.getTable(baseToken);
        }
    }

    /**
     * Restarts the view over an emptied catalogue, and deliberately does not call
     * {@code hydrateAllTables()} afterwards - the point is to leave the refresh worker facing
     * the cache the startup hydrator has not filled yet.
     */
    private void restartWithAColdCatalogue() {
        engine.getLiveViewRegistry().clear();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactive();
        try (MetadataCacheWriter cacheRW = engine.getMetadataCache().writeLock()) {
            cacheRW.clearCache();
        }
        engine.buildViewGraphs();
    }

    private void seedFiveBoundaries() throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY HOUR WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum "
                + "FROM tx WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");

        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            for (int second = 0; second <= 40; second += 10) {
                execute("INSERT INTO tx VALUES ('" + timestamp(second) + "', '"
                        + (second % 20 == 0 ? "acct-1" : "acct-2") + "', " + (second + 1.0) + ")");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
        }

        Assert.assertEquals("the seed must leave one boundary per commit", BOUNDARIES, countSealedBoundaries("lv"));
        assertNoRefreshFaults("lv");
    }
}
