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
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * A BACKFILL live view reads base partitions through a page-frame cursor. When
 * those partitions are parquet and the view carries a WHERE clause, the filter is
 * pushed down to the parquet row-group level, so a fully non-matching row group is
 * pruned before the scan yields any of its rows. The backfill sweep resumes across
 * turns with {@code skipRows()}; that skip must land on the same row the pruned scan
 * next yields, or the sweep re-reads already-consumed rows (double-counted output).
 * This suite pins that equivalence.
 */
public class LiveViewParquetBaseTest extends AbstractCairoTest {

    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testBackfillOverMixedParquetNativeBaseMatchesRecompute() throws Exception {
        // A base with a leading run of parquet partitions (some fully pruned by the
        // WHERE) followed by native partitions. Exercises the skip walk crossing the
        // parquet/native boundary mid-sweep.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Days 1-3 all i <= 0 (each a fully non-matching row group), days 4-6 mixed.
            final StringBuilder sb = new StringBuilder("INSERT INTO base (ts, sym, i) VALUES ");
            int total = 0;
            for (int day = 1; day <= 6; day++) {
                int rows = 2 + (day % 3);
                for (int r = 0; r < rows; r++) {
                    if (total > 0) {
                        sb.append(", ");
                    }
                    long iv = day <= 3 ? -(r + 1) : ((r % 2 == 0) ? (day * 10 + r) : -(r + 1));
                    sb.append("('2026-01-0").append(day).append("T00:00:0").append(r).append(".000000Z', '")
                            .append(r % 2 == 0 ? "a" : "b").append("', ").append(iv).append(")");
                    total++;
                }
            }
            execute(sb.toString());
            drainWalQueue();
            // Convert days 1-4 to parquet (days 1-3 fully pruned, day 4 partially).
            execute("ALTER TABLE base CONVERT PARTITION TO PARQUET WHERE ts < '2026-01-05'");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms BACKFILL AS " +
                    "SELECT ts, sym, i, sum(i) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS v " +
                    "FROM base WHERE i > 0");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveBackfillToCompletion(job, "lv");
            }
            drainWalQueue();

            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(SELECT ts, sym, i, sum(i) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS v FROM base WHERE i > 0) ORDER BY 1, 2",
                    "(lv) ORDER BY 1, 2",
                    LOG,
                    true
            );
            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testBackfillOverParquetBaseMatchesRecompute() throws Exception {
        // A small per-turn budget forces the backfill to resume with skipRows()
        // many times over a base whose leading partitions are fully pruned by the
        // WHERE. Before the fix the pruned rows were counted by the skip, landing it
        // short and duplicating rows in the view.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Days 1-4: all i <= 0 (each a fully non-matching row group). Days 5-9: all i > 0.
            final StringBuilder sb = new StringBuilder("INSERT INTO base (ts, sym, i) VALUES ");
            int total = 0;
            for (int day = 1; day <= 9; day++) {
                int rows = 2 + (day % 3);
                for (int r = 0; r < rows; r++) {
                    if (total > 0) {
                        sb.append(", ");
                    }
                    long iv = day <= 4 ? -(r + 1) : (day * 10 + r + 1);
                    sb.append("('2026-01-0").append(day).append("T00:00:0").append(r).append(".000000Z', '")
                            .append(r % 2 == 0 ? "a" : "b").append("', ").append(iv).append(")");
                    total++;
                }
            }
            execute(sb.toString());
            drainWalQueue();
            // Convert all settled partitions (days 1-8) to parquet before CREATE.
            execute("ALTER TABLE base CONVERT PARTITION TO PARQUET WHERE ts < '2026-01-09'");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms BACKFILL AS " +
                    "SELECT ts, sym, i, sum(i) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS v " +
                    "FROM base WHERE i > 0");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveBackfillToCompletion(job, "lv");
            }
            drainWalQueue();

            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(SELECT ts, sym, i, sum(i) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS v FROM base WHERE i > 0) ORDER BY 1, 2",
                    "(lv) ORDER BY 1, 2",
                    LOG,
                    true
            );
            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testLimitWhereSkipOverParquetIsStable() throws Exception {
        // A plain LIMIT lo,hi with a pushdown-filterable WHERE over a parquet base
        // routes skipRows() through the filtered cursor's row-by-row path, so it was
        // already correct; this locks that no regression sneaks into that path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, i LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, i) VALUES " +
                    "('2026-01-01T00:00:00.000000Z', -5), ('2026-01-01T00:00:01.000000Z', -3), " +
                    "('2026-01-02T00:00:00.000000Z', 10), ('2026-01-02T00:00:01.000000Z', 20), " +
                    "('2026-01-02T00:00:02.000000Z', 30), ('2026-01-03T00:00:00.000000Z', -1), " +
                    "('2026-01-03T00:00:01.000000Z', 40), ('2026-01-03T00:00:02.000000Z', 50)");
            drainWalQueue();
            execute("ALTER TABLE base CONVERT PARTITION TO PARQUET WHERE ts < '2026-01-03'");
            drainWalQueue();

            // 5 matching rows: 10, 20, 30, 40, 50. Skip 2 -> 30, 40, 50.
            assertQuery("SELECT i FROM base WHERE i > 0 LIMIT 2,5")
                    .noLeakCheck()
                    .returns("i\n30\n40\n50\n");
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

    private void driveBackfillToCompletion(LiveViewRefreshJob job, String viewName) {
        for (int i = 0; i < 2000; i++) {
            LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance(viewName);
            if (inst == null
                    || inst.getStateReader().getBackfillState() != LiveViewState.BACKFILL_STATE_BACKFILLING) {
                break;
            }
            drainJob(job);
        }
        drainWalQueue();
    }
}
