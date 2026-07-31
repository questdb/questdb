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

package io.questdb.test.cairo.sql.async;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.async.PageFrameReduceDispatcher;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

/**
 * Diagnostic comparison of per-frame scheduling cost across execution modes, printed rather than
 * asserted: legacy inline reduce, then fiber launch at each batch limit in the sweep. The query
 * runs single-threaded through the owner work-steal path, so every frame's scheduling machinery
 * executes synchronously and the timing differences are attributable to it.
 * <p>
 * Small frames come from the page-frame-max-rows override; the batch row budget is pinned to the
 * production default so the sweep models small actual frames (e.g. small partitions) under a
 * default-configured deployment.
 */
public class PageFrameFiberLaunchOverheadTest extends AbstractCairoTest {
    private static final int[] BATCH_SWEEP = {1, 4, 16, 64, 256};
    private static final int FRAME_ROWS = 1_000;
    private static final int MEASURE_RUNS = 15;
    private static final int ROW_COUNT = 1_000_000;
    private static final int WARMUP_RUNS = 5;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, FRAME_ROWS);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 1);
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testCompareSchedulingOverhead() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE tab AS (" +
                            " SELECT x, timestamp_sequence(0, 1_000) AS ts" +
                            " FROM long_sequence(" + ROW_COUNT + ")" +
                            ") TIMESTAMP(ts)"
            );
            runComparison("ordered", "SELECT count(*) FROM tab WHERE x > 0", 1);
        });
    }

    @Test
    public void testCompareSchedulingOverheadUnordered() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE tab AS (" +
                            " SELECT x, x % 16 AS k, timestamp_sequence(0, 1_000) AS ts" +
                            " FROM long_sequence(" + ROW_COUNT + ")" +
                            ") TIMESTAMP(ts)"
            );
            runComparison("unordered", "SELECT k, count(*) FROM tab GROUP BY k", 16);
        });
    }

    private long measureRows(String label, String sql, int frameCount, int expectedRowCount) throws Exception {
        final long[] samples = new long[MEASURE_RUNS];
        try (RecordCursorFactory factory = select(sql)) {
            for (int i = 0; i < WARMUP_RUNS; i++) {
                runOnce(factory, expectedRowCount);
            }
            for (int i = 0; i < MEASURE_RUNS; i++) {
                final long start = System.nanoTime();
                runOnce(factory, expectedRowCount);
                samples[i] = System.nanoTime() - start;
            }
        }
        Arrays.sort(samples);
        final long median = samples[MEASURE_RUNS / 2];
        final long best = samples[0];
        System.out.printf(
                "%s: median %.3fms, best %.3fms, per-frame %dns (median)%n",
                label,
                median / 1_000_000.0,
                best / 1_000_000.0,
                median / frameCount
        );
        return median;
    }

    private void runComparison(String pathLabel, String sql, int expectedRowCount) throws Exception {
        final int frameCount = ROW_COUNT / FRAME_ROWS;
        final long legacyNanos = measureRows(
                String.format("LEGACY inline   (%s)", pathLabel), sql, frameCount, expectedRowCount);

        final FiberRuntime runtime = new FiberRuntime(4);
        final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                engine,
                engine.getMessageBus(),
                runtime
        );
        try {
            engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
            dispatcher.setBatchRowBudgetForTesting(1_000_000);

            final long[] fiberNanos = new long[BATCH_SWEEP.length];
            for (int i = 0; i < BATCH_SWEEP.length; i++) {
                dispatcher.setBatchLimitForTesting(BATCH_SWEEP[i]);
                fiberNanos[i] = measureRows(
                        String.format("FIBER batch=%-3d (%s)", BATCH_SWEEP[i], pathLabel),
                        sql, frameCount, expectedRowCount);
            }

            final StringBuilder deltas = new StringBuilder();
            deltas.append(String.format("%n%s per-frame scheduling delta vs LEGACY:", pathLabel));
            for (int i = 0; i < BATCH_SWEEP.length; i++) {
                deltas.append(String.format(
                        " batch=%d %+dns", BATCH_SWEEP[i], (fiberNanos[i] - legacyNanos) / frameCount));
            }
            System.out.println(deltas);
        } finally {
            runtime.beginQuiesce();
            final long deadline = System.nanoTime() + 5_000_000_000L;
            while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
                runtime.drain(64);
            }
            Assert.assertTrue(runtime.awaitClosed(deadline));
            runtime.closeAfterDrained();
            Misc.free(dispatcher);
        }
    }

    private void runOnce(RecordCursorFactory factory, int expectedRowCount) throws Exception {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            int rowCount = 0;
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                if (expectedRowCount == 1) {
                    Assert.assertEquals(ROW_COUNT, record.getLong(0));
                }
                rowCount++;
            }
            Assert.assertEquals(expectedRowCount, rowCount);
        }
    }
}
