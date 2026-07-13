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
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.mp.Job;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The BACKFILL sweep is multi-turn: it yields on a per-turn row/duration budget and
 * resumes positionally with {@code skipRows(dataOffset)}. A base page-frame cursor
 * yields rows in physical ts-sorted order, and an out-of-order base apply merge-
 * rewrites the partition to preserve that order. If the sweep re-opened the base at
 * the latest applied seqTxn each turn, an out-of-order (back-dated) commit landing
 * below the swept prefix <em>between</em> turns would shift every later row up by
 * one: the next {@code skipRows(dataOffset)} would skip a different set, silently
 * dropping the back-dated row and re-feeding the old boundary row (a duplicate that
 * also double-advances the window accumulators).
 * <p>
 * The fix pins ONE base snapshot for the whole sweep, so every turn reads the same
 * physical order. Everything the base commits after the snapshot is left to the
 * ACTIVE phase's O3 detection (from {@code sweepSeqTxn + 1}). This suite injects a
 * back-dated commit between two sweep turns and asserts the view still equals a
 * from-scratch recompute over the final base.
 */
public class LiveViewBackfillO3Test extends AbstractLiveViewTest {

    @Before
    public void pinClockBelowTestData() {
        // Pin the test clock below all test data so the ACTIVE phase's lower-bound floor
        // (the BACKFILL global-min timestamp) accepts the back-dated row injected mid-sweep.
        setCurrentMicros(0L);
    }

    @Test
    public void testBackfillO3BelowSweptPrefixMatchesRecompute() throws Exception {
        // One row per turn: the sweep yields after every base row, so we can stop it
        // mid-sweep and inject a back-dated commit before the remaining turns run.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Six in-order rows, all in one partition (day 2026-01-01) so a later
            // back-dated insert O3-merges into the same partition and reorders it.
            execute("INSERT INTO base (ts, sym, x) VALUES " +
                    "('2026-01-01T00:00:10.000000Z', 'a', 10), " +
                    "('2026-01-01T00:00:20.000000Z', 'a', 20), " +
                    "('2026-01-01T00:00:30.000000Z', 'a', 30), " +
                    "('2026-01-01T00:00:40.000000Z', 'a', 40), " +
                    "('2026-01-01T00:00:50.000000Z', 'a', 50), " +
                    "('2026-01-01T00:01:00.000000Z', 'a', 60)");
            drainWalQueue();

            final String viewSql = "SELECT ts, sym, x, " +
                    "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS v " +
                    "FROM base";
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms BACKFILL AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(inst);

                // Drive exactly three sweep turns (ts 10, 20, 30 swept; dataOffset == 3),
                // stopping while the view is still BACKFILLING.
                for (int i = 0; i < 100 && inst.getBackfillDataOffset() < 3; i++) {
                    if (inst.getStateReader().getBackfillState() != LiveViewState.BACKFILL_STATE_BACKFILLING) {
                        break;
                    }
                    job.run();
                }
                Assert.assertEquals(
                        "the sweep must still be BACKFILLING when the back-dated row is injected",
                        LiveViewState.BACKFILL_STATE_BACKFILLING,
                        inst.getStateReader().getBackfillState()
                );
                Assert.assertEquals(
                        "exactly the leading prefix must be swept before injection",
                        3,
                        inst.getBackfillDataOffset()
                );

                // Inject a back-dated row BELOW the swept prefix (ts 15 < swept max ts 30, above
                // the floor ts 10). Its apply O3-merges into the partition, shifting ts 20/30/...
                // up by one physical position. With the pre-fix positional resume this made the
                // next skipRows(3) skip ts 15 (silent loss) and re-feed ts 30 (duplicate).
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-01-01T00:00:15.000000Z', 'a', 15)");
                drainWalQueue();

                // Finish the sweep off the pinned snapshot, then let the ACTIVE phase drain and
                // materialise the back-dated row via its O3 detection.
                for (int i = 0; i < 2000; i++) {
                    if (inst.getStateReader().getBackfillState() != LiveViewState.BACKFILL_STATE_BACKFILLING) {
                        break;
                    }
                    job.run();
                }
                for (int i = 0; i < 200; i++) {
                    job.run();
                }
            }
            drainWalQueue();

            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(" + viewSql + ") ORDER BY 1, 2",
                    "(lv) ORDER BY 1, 2",
                    LOG,
                    true
            );
            assertNoRefreshFaults("lv");

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }
}
