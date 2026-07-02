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

import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.mp.Job;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * DDL-on-base-table behaviour for live views. Focuses on schema changes that are
 * routed through {@code ApplyWal2TableJob}'s structural path and reach
 * {@code CairoEngine.invalidateLiveViewsForBaseSchemaChange}.
 * <p>
 * The centrepiece is {@code ALTER COLUMN TYPE} on a referenced column. The refresh
 * path derives each column's stride from the cached compile-time factory metadata,
 * so a referenced column whose type changed under the view would be read through the
 * stale stride: wrong results on a widening change, and an out-of-bounds native read
 * (SIGSEGV / corruption) on a narrowing or fixed&lt;-&gt;var-size change. The view
 * must therefore flip to INVALID before it ever refreshes over the changed base. A
 * type change to a column the view does not read must stay transparent.
 */
public class LiveViewBaseDdlTest extends AbstractCairoTest {

    // Pin the test clock below all test data before each test. A non-BACKFILL view's
    // lower bound is the CREATE wall-clock moment, and the forward-append refresh path
    // drops rows below it. The test data is timestamped in the past, so without a
    // pinned clock every row would be dropped as pre-CREATE.
    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testAlterReferencedColumnTypeInvalidatesLiveView() throws Exception {
        // Three transitions exercise the three failure modes the stale stride would
        // produce if the change were not caught:
        //   INT->LONG    - widening, old stride < new data: in-bounds under-read (wrong results).
        //   LONG->INT    - narrowing, old stride > new data: out-of-bounds native read.
        //   INT->VARCHAR - fixed->var-size: the record reads var-size aux offsets over
        //                  fixed bytes - a wild pointer.
        // In every direction the referenced-column type change must invalidate the view
        // with the "change column type operation" reason, so no refresh ever runs over
        // the changed base.
        assertReferencedColumnTypeChangeInvalidates("INT", "LONG");
        assertReferencedColumnTypeChangeInvalidates("LONG", "INT");
        assertReferencedColumnTypeChangeInvalidates("INT", "VARCHAR");
    }

    @Test
    public void testAlterUnreferencedColumnTypeIsTransparent() throws Exception {
        // Changing the TYPE of a column the LV never reads must NOT invalidate it, and
        // the view must keep refreshing correctly across the change (documents the
        // boundary of the type-change invalidation and proves it is not over-broad).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, y INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x, y) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 10, 1), " +
                        "('2026-01-01T00:00:02.000000Z', 20, 2)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertFalse("LV must start valid", instance.isInvalid());

                // Change the type of y, which the view never reads.
                setCurrentMicros(2_000_000L);
                execute("ALTER TABLE base ALTER COLUMN y TYPE LONG");
                drainWalQueue();
                Assert.assertFalse(
                        "changing the type of an unreferenced column must not invalidate the LV",
                        instance.isInvalid()
                );

                // Post-change ingestion must keep refreshing correctly.
                setCurrentMicros(4_000_000L);
                execute("INSERT INTO base (ts, x, y) VALUES " +
                        "('2026-01-01T00:00:03.000000Z', 30, 3), " +
                        "('2026-01-01T00:00:04.000000Z', 40, 4)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertFalse("LV must stay valid after unreferenced type change", instance.isInvalid());
            }

            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:01.000000Z\t10\t1\n" +
                            "2026-01-01T00:00:02.000000Z\t20\t2\n" +
                            "2026-01-01T00:00:03.000000Z\t30\t3\n" +
                            "2026-01-01T00:00:04.000000Z\t40\t4\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    private void assertReferencedColumnTypeChangeInvalidates(String initialType, String newType) throws Exception {
        final String transition = initialType + "->" + newType;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x " + initialType + ", y INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertFalse("LV must start valid [" + transition + ']', instance.isInvalid());

            execute("ALTER TABLE base ALTER COLUMN x TYPE " + newType);
            drainWalQueue();

            Assert.assertTrue(
                    "changing the type of a referenced column must invalidate the LV [" + transition + ']',
                    instance.isInvalid()
            );
            Assert.assertTrue(
                    "wrong invalidation reason [" + transition + ", reason=" + instance.getInvalidationReason() + ']',
                    Chars.contains(instance.getInvalidationReason(), "change column type operation")
            );

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    private static boolean drainJob(Job job) {
        boolean any = false;
        for (int i = 0; i < 64 && job.run(); i++) {
            any = true;
        }
        return any;
    }
}
