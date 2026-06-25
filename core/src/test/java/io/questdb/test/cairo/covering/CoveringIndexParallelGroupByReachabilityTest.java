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
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * F1 reachability regression for the level-3 review of the parallel covered-index
 * decode PR. Proves that a covering-index source CAN be the base of an async keyed
 * GROUP BY, i.e. that covered frames are dispatched through {@code
 * UnorderedPageFrameSequence} — the sequence that, before this PR's fix, did NOT
 * freeze the per-partition posting readers before worker decode (a concurrent writer
 * could then remap valueMem under in-flight worker addresses: use-after-free).
 * <p>
 * The structural assertion is sufficient: {@code AsyncGroupByRecordCursorFactory}
 * drives its scan exclusively through {@code UnorderedPageFrameSequence}, so a
 * covering source proven to sit beneath it necessarily has its covered frames
 * dispatched there. (A throwaway run with an instrumented dispatch counter confirmed
 * the runtime path directly.) The result equality additionally proves the freeze
 * wired into that sequence yields correct answers.
 */
public class CoveringIndexParallelGroupByReachabilityTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        // Many small page frames so the covered scan fans out across the worker pool.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
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
