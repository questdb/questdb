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
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Live view refresh with the decoded checkpoint page cache switched off
 * engine-wide.
 * <p>
 * {@code cairo.live.view.checkpoint.page.cache.max.bytes = 0} leaves every view
 * without a cache at all, rather than with one that can never admit, so the
 * restore path skips the probe and everything that reports on the cache has to
 * cope with there being none. The cap is read once, when the engine builds its
 * {@code LiveViewRegistry}, which is why this needs a class of its own: the
 * suite's other live view tests all run with the default 256 MB budget and would
 * never take these branches.
 */
public class LiveViewCheckpointPageCacheDisabledTest extends AbstractLiveViewTest {

    private static final String PAGE_CACHE_QUERY = "SELECT checkpoint_page_cache_bytes, "
            + "checkpoint_page_cache_working_set_bytes, checkpoint_page_cache_hits, "
            + "checkpoint_page_cache_misses, checkpoint_page_cache_admission_ratio FROM live_views()";
    private static final String VIEW_SQL = "SELECT ts, sym, sum(x) OVER w AS s FROM base "
            + "WINDOW w AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW)";

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Before the engine is built: the registry takes the cap in CairoEngine's
        // constructor and holds it for the engine's life, so setting this from
        // inside a test would leave the budget enabled.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_PAGE_CACHE_MAX_BYTES, 0);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testResumeReplayReportsNoPageCacheAndStillMatchesRecompute() throws Exception {
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + VIEW_SQL);

            capture.start();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Four in-order commits lay down the boundaries a resume can anchor
                // on, then three corrections just below the head resume from one of
                // them - the same shape the cached case is measured on, so the two
                // differ in the budget and nothing else.
                for (int second = 10; second <= 40; second += 10) {
                    commitRow(job, second, second);
                }
                commitRow(job, 35, 350);
                commitRow(job, 37, 370);
                commitRow(job, 39, 390);

                final LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                Assert.assertTrue(
                        "the view never resumed from an anchor, so it restored no checkpoint and the"
                                + " assertions below would pass over a path that never ran",
                        lv.getO3ResumeReplayRows() > 0
                );
                Assert.assertNull(
                        "a disabled budget must leave the view with no cache, not an empty one",
                        lv.getCheckpointPageCache()
                );

                // The log is written asynchronously, so plant a sentinel behind the
                // last replay line and wait for it: one FIFO path means every
                // earlier line is in the sink by the time this one is.
                LOG.info().$("live view page cache disabled flush barrier").$();
                capture.waitForRegex("live view page cache disabled flush barrier");
                capture.assertLoggedRE("live view O3 resume replay completed \\[view=lv, .*, rowsEmitted=\\d+]");
                // The counters end where the line ends, so their absence is what
                // distinguishes "no cache" from a cold cache reporting zeroes.
                capture.assertNotLogged("pageCacheHits=");
                capture.assertNotLogged("pageCacheMisses=");
            } finally {
                capture.stop();
            }

            // The catalogue applies the same rule to the same state: NULL covers a
            // view whose engine has the budget off, not just one that has not
            // restored yet.
            assertQuery(PAGE_CACHE_QUERY).noLeakCheck().noRandomAccess()
                    .returns("checkpoint_page_cache_bytes\tcheckpoint_page_cache_working_set_bytes\t"
                            + "checkpoint_page_cache_hits\tcheckpoint_page_cache_misses\t"
                            + "checkpoint_page_cache_admission_ratio\n"
                            + "null\tnull\tnull\tnull\tnull\n");

            // The cache-off arm of the differential, at engine scope: a restore that
            // decodes every page must land the same view the cached suite asserts.
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(" + VIEW_SQL + ") ORDER BY 2, 1",
                    "(lv) ORDER BY 2, 1",
                    LOG,
                    true
            );
            assertNoRefreshFaults("lv");

            execute("DROP LIVE VIEW lv");
        });
    }

    /**
     * Commits one row per key at {@code second} and gives the refresh job a turn on
     * it. The clock steps past the view's flush window first, so the commit reaches
     * disk and seals a boundary rather than lingering as an unflushed lead the next
     * commit would absorb.
     */
    private void commitRow(LiveViewRefreshJob job, int second, double value) throws Exception {
        setCurrentMicros(currentMicros + 200_000L);
        final String rowTs = "2026-11-01T00:00:" + (second < 10 ? "0" + second : second) + ".000000Z";
        execute("INSERT INTO base (ts, sym, x) VALUES "
                + "('" + rowTs + "', 'a', " + value + "), "
                + "('" + rowTs + "', 'b', " + (value + 1) + ")");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }
}
