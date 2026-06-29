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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import io.questdb.cairo.security.AllowAllSecurityContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * F1 regression for the level-3 review of the parallel covered-index decode PR.
 * <p>
 * The first test proves a covering-index source CAN be the base of an async keyed
 * GROUP BY, i.e. that covered frames are dispatched through {@code
 * UnorderedPageFrameSequence} — the sequence that, before this PR's fix, did not
 * freeze the per-partition posting readers before worker decode. The structural
 * assertion is sufficient: {@code AsyncGroupByRecordCursorFactory} drives its scan
 * exclusively through {@code UnorderedPageFrameSequence}, so a covering source proven
 * to sit beneath it necessarily has its covered frames dispatched there. (A throwaway
 * instrumented dispatch counter confirmed the runtime path directly.)
 * <p>
 * The second test stresses that path with a concurrent writer and four workers.
 * Investigation note: removing the freeze does NOT corrupt this path — the eager
 * production warm makes worker decode read-only and the query reader is snapshot-stable
 * with valueMem pre-extended — so F1 is a latent/defensive gap on this path rather than
 * a live use-after-free. The freeze is nonetheless correct: it makes the unordered
 * sequence consistent with the base {@code PageFrameSequence} (which freezes), keeps
 * the frozen-only {@code openRequiredSidecars} no-op (F3) effective, and future-proofs
 * against any change that lets the reader advance mid-query or leaves a gen cold.
 */
public class CoveringIndexParallelGroupByReachabilityTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        // Many small page frames so the covered scan fans out across the worker pool.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        // Bind a real (generous, never-breached) per-query memory tracker so covered decode
        // buffers charge it — this exercises the M1 accounting path and, under -ea, makes the
        // tracker-recycle guard (PerQueryMemoryTracker.acquire asserts used==0) catch any
        // cross-query mis-charge by the captured CoveringBuffers.bufferTracker.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 512 * 1024 * 1024L);
        super.setUp();
    }

    @Test
    public void testKeyedParallelGroupByOverCoveringIndexDispatchesCoveredFrames() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        // Covering index: filter on the indexed SYMBOL, GROUP BY a covered
                        // VARCHAR. A VARCHAR key is not vectorizable, so this routes through the
                        // async AsyncGroupByRecordCursorFactory (-> UnorderedPageFrameSequence),
                        // NOT the vectorized GroupByRecordCursorFactory whose base sequence
                        // already freezes covered readers.
                        engine.execute(
                                "CREATE TABLE t (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL INDEX TYPE POSTING INCLUDE (grp, payload)," +
                                        "  grp VARCHAR," +
                                        "  payload LONG" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        // Non-indexed twin for the reference result.
                        engine.execute(
                                "CREATE TABLE ref (ts TIMESTAMP, sym SYMBOL, grp VARCHAR, payload LONG)" +
                                        " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        final String insert =
                                "SELECT (x * 600000000L)::timestamp," +
                                        " CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END," +
                                        " 'g' || (x % 7)," +
                                        " x" +
                                        " FROM long_sequence(20000)";
                        engine.execute("INSERT INTO t " + insert, sqlExecutionContext);
                        engine.execute("INSERT INTO ref " + insert, sqlExecutionContext);
                        engine.releaseAllWriters();

                        final String coveredSql = "SELECT grp, sum(payload) FROM t WHERE sym = 'A' GROUP BY grp ORDER BY grp";
                        final String refSql = "SELECT grp, sum(payload) FROM ref WHERE sym = 'A' GROUP BY grp ORDER BY grp";

                        // 1) The plan really is an async keyed GROUP BY OVER a covering source:
                        // covered frames therefore flow through UnorderedPageFrameSequence (the
                        // path that had no freeze before F1).
                        try (RecordCursorFactory factory = compiler.compile(coveredSql, sqlExecutionContext).getRecordCursorFactory()) {
                            assertInTree(factory, AsyncGroupByRecordCursorFactory.class);
                            assertInTree(factory, CoveringIndexRecordCursorFactory.class);
                        }

                        // 2) ... and executing it matches the non-indexed reference, proving the
                        // freeze wired into that sequence yields correct answers under workers.
                        TestUtils.assertSqlCursors(engine, sqlExecutionContext, refSql, coveredSql, LOG);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testKeyedParallelGroupByOverCoveringIndexSurvivesConcurrentWriter() throws Exception {
        // Concurrency regression for the covered async keyed GROUP BY (the path F1 hardens).
        //
        // The sym='A' covered data is FIXED (payload = 1, so each group's sum is its row
        // count) -> the covered result is a constant the whole test long. A background
        // writer continuously O3-inserts sym='B' rows INTO THE SAME PARTITIONS, so every
        // commit extends/republishes the shared per-partition posting-index valueMem that
        // the 'A' decode reads while 4 workers decode covered frames concurrently. The
        // 'A' result must ALWAYS equal the captured snapshot.
        //
        // NOTE: this is a positive regression guard, NOT an F1 negative control. Removing
        // the dispatch freeze does not make it fail — the eager production warm already
        // makes worker decode replay read-only and the query reader is snapshot-stable with
        // valueMem pre-extended, so the missing freeze is a latent/defensive gap here rather
        // than a live use-after-free (verified by running this with the freeze removed). The
        // freeze remains correct hardening: it matches the base PageFrameSequence path and
        // keeps F3's frozen-only openRequiredSidecars no-op effective.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE t (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL INDEX TYPE POSTING INCLUDE (grp, payload)," +
                                        "  grp VARCHAR," +
                                        "  payload LONG" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        // Fixed sym='A' covered data: 8000 rows over ~18 day-partitions,
                        // payload = 1, 4 groups -> each group's sum == 2000.
                        engine.execute(
                                "INSERT INTO t SELECT (x * 200000000L)::timestamp, 'A', 'g' || (x % 4), 1" +
                                        " FROM long_sequence(8000)",
                                sqlExecutionContext
                        );
                        engine.releaseAllWriters();

                        final String coveredSql = "SELECT grp, sum(payload) FROM t WHERE sym = 'A' GROUP BY grp ORDER BY grp";

                        // Capture the exact, constant expected result (no writer running yet).
                        final StringSink expected = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, coveredSql, expected);
                        final String expectedStr = expected.toString();

                        final AtomicBoolean stop = new AtomicBoolean(false);
                        final AtomicReference<Throwable> writerError = new AtomicReference<>();
                        final Thread writer = new Thread(() -> {
                            try (SqlExecutionContextImpl wctx = new SqlExecutionContextImpl(engine, 1)) {
                                wctx.with(AllowAllSecurityContext.INSTANCE);
                                while (!stop.get()) {
                                    // Large O3 batch across the SAME full partition range as 'A'
                                    // (offset interleaves the timestamps) so every commit grows and
                                    // republishes the per-partition posting index valueMem that 'A'
                                    // decode reads — maximizing the mid-decode remap window.
                                    engine.execute(
                                            "INSERT INTO t SELECT (x * 200000000L + 100000000L)::timestamp, 'B', 'b', 100" +
                                                    " FROM long_sequence(8000)",
                                            wctx
                                    );
                                }
                            } catch (Throwable th) {
                                writerError.compareAndSet(null, th);
                            } finally {
                                // This thread is spawned by the test, not the harness, so it must
                                // free its own native thread-locals (the per-thread Path and the
                                // O3 merge scratch) or the leak check trips on NATIVE_PATH.
                                Path.clearThreadLocals();
                                Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                            }
                        }, "covering-f1-writer");
                        writer.start();
                        try {
                            final StringSink actual = new StringSink();
                            for (int i = 0; i < 400 && writerError.get() == null; i++) {
                                TestUtils.printSql(engine, sqlExecutionContext, coveredSql, actual);
                                Assert.assertEquals(
                                        "covered sym='A' result must be stable under a concurrent writer (F1) at iteration " + i,
                                        expectedStr,
                                        actual.toString()
                                );
                            }
                        } finally {
                            stop.set(true);
                            writer.join();
                        }
                        if (writerError.get() != null) {
                            throw new AssertionError("background writer failed", writerError.get());
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testCoveredAsyncGroupByTrackerBalancedAcrossManyQueries() throws Exception {
        // Regression for the covered-buffer / per-query-tracker lifetime: run the covered async
        // keyed GROUP BY many times under a real per-query MemoryTracker so the tracker is
        // acquired, used, released, and recycled repeatedly. Covered decode buffers are
        // query-lifetime and are freed only at the NEXT query's clear()/of() — AFTER the owning
        // query's tracker has been recycled. They therefore must NOT be charged to that tracker
        // (they use global NATIVE_INDEX_READER accounting); if they were, the deferred free would
        // decrement a recycled block and PerQueryMemoryTracker.acquire()'s `assert getUsed() == 0`
        // would trip on a later query. A clean run confirms covered buffers stay off the per-query
        // tracker and the limit path is leak-free.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE t (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL INDEX TYPE POSTING INCLUDE (grp, payload)," +
                                        "  grp VARCHAR," +
                                        "  payload LONG" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO t SELECT (x * 200000000L)::timestamp, 'A', 'g' || (x % 4), x" +
                                        " FROM long_sequence(8000)",
                                sqlExecutionContext
                        );
                        engine.releaseAllWriters();

                        final String coveredSql = "SELECT grp, sum(payload) FROM t WHERE sym = 'A' GROUP BY grp ORDER BY grp";
                        final StringSink expected = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, coveredSql, expected);
                        final String expectedStr = expected.toString();

                        final StringSink actual = new StringSink();
                        for (int i = 0; i < 50; i++) {
                            TestUtils.printSql(engine, sqlExecutionContext, coveredSql, actual);
                            Assert.assertEquals("covered result must be stable across queries at iteration " + i,
                                    expectedStr, actual.toString());
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private static void assertInTree(RecordCursorFactory factory, Class<?> expected) {
        RecordCursorFactory f = factory;
        while (f != null) {
            if (expected.isInstance(f)) {
                return;
            }
            f = f.getBaseFactory();
        }
        Assert.fail("expected " + expected.getSimpleName() + " in the factory tree, but top was " + factory.getClass().getName());
    }
}
