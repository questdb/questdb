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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.PropertyKey;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression guard for {@code first_not_null} / {@code last_not_null} on the non-sharded batched
 * aggregation path, which a stock server uses by default.
 * <p>
 * A parallel GROUP BY worker reduces a whole page frame at once through
 * {@link io.questdb.griffin.engine.functions.GroupByFunction#computeKeyedBatch computeKeyedBatch}
 * rather than row by row, as long as its map fragment has not sharded
 * ({@code AsyncGroupByRecordCursorFactory.aggregateNonShardedBatched}). The not-null aggregators
 * inherit the plain first/last {@code computeKeyedBatch}, which picks the winning row by rowId
 * alone: {@code last} takes the max rowId and writes its value unconditionally, so a trailing NULL
 * clobbers the real value; {@code first} takes the min rowId and never revisits the slot, so a
 * leading NULL is stored and then kept. Every not-null type must therefore override
 * {@code computeKeyedBatch} to skip NULL input and to overwrite a stored NULL. IPv4 was the one
 * type that did not, in both First and Last, and returned NULL for keys whose value it should have
 * found.
 * <p>
 * {@link FirstLastNotNullParallelMergeTest} cannot cover this: it pins the sharding threshold to 1,
 * which forces the sharded per-row path and bypasses {@code computeKeyedBatch} entirely. This class
 * pins the threshold high instead, so the fragments never shard and every frame goes through the
 * batched path.
 * <p>
 * Each key holds exactly one non-null value and many NULLs, placed where the buggy comparison drops
 * it: at the key's first (lowest-rowId) occurrence for {@code last_not_null}, so the NULLs that
 * follow carry higher rowIds, and at its last (highest-rowId) occurrence for {@code first_not_null},
 * so the NULLs that precede it carry lower rowIds. A correct aggregate is non-null for all
 * KEY_COUNT keys, so each count must equal KEY_COUNT. INT columns ride along as a control: INT
 * already overrides {@code computeKeyedBatch}, so its counts stay at KEY_COUNT either way and a
 * failure that moves all four counts points at the harness rather than at the aggregators.
 */
public class FirstLastNotNullBatchedGroupByTest extends AbstractCairoTest {

    private static final int KEY_COUNT = 100;
    private static final int ROW_COUNT = 10_000;
    // last_not_null: non-null at each key's first (lowest-rowId) occurrence.
    private static final String FIRST_OCCURRENCE = "x <= " + KEY_COUNT;
    // first_not_null: non-null at each key's last (highest-rowId) occurrence.
    private static final String LAST_OCCURRENCE = "x > " + (ROW_COUNT - KEY_COUNT);

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        // Keep the fragments unsharded, so the reducer stays on the batched path that
        // computeKeyedBatch serves. This is what a stock server does; the sibling merge test pins
        // this to 1 to force the opposite path.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1_000_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        // Small frames, so a key's rows scatter across many frames and every worker map sees both
        // the key's non-null row and its NULL rows.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 64);
        super.setUp();
    }

    @Test
    public void testNotNullKeyedBatchKeepsNonNull() throws Exception {
        final String createSql = "CREATE TABLE tab AS (" +
                "  SELECT (x % " + KEY_COUNT + ")::int AS g," +
                "  CASE WHEN " + FIRST_OCCURRENCE + " THEN ipv4 '10.0.0.1' END AS vFirstIPv4," +
                "  CASE WHEN " + LAST_OCCURRENCE + " THEN ipv4 '10.0.0.1' END AS vLastIPv4," +
                "  CASE WHEN " + FIRST_OCCURRENCE + " THEN 42::int END AS vFirstInt," +
                "  CASE WHEN " + LAST_OCCURRENCE + " THEN 42::int END AS vLastInt" +
                "  FROM long_sequence(" + ROW_COUNT + ")" +
                ")";
        final String query = "SELECT count(aLastIPv4) LastIPv4, count(aFirstIPv4) FirstIPv4," +
                " count(aLastInt) LastInt, count(aFirstInt) FirstInt FROM (" +
                "SELECT g," +
                " last_not_null(vFirstIPv4) aLastIPv4," +
                " first_not_null(vLastIPv4) aFirstIPv4," +
                " last_not_null(vFirstInt) aLastInt," +
                " first_not_null(vLastInt) aFirstInt" +
                " FROM tab)";

        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (ignore, compiler, ctx) -> {
                    execute(compiler, createSql, ctx);
                    // Without the parallel factory there is no batched reduce left to guard.
                    TestUtils.printSql(compiler, ctx, "EXPLAIN " + query, sink);
                    TestUtils.assertContains(sink, "Async Group By");
                    assertQuery(query)
                            .noLeakCheck()
                            .withCompiler(compiler)
                            .withContext(ctx)
                            .noRandomAccess()
                            .expectSize()
                            .returns("LastIPv4\tFirstIPv4\tLastInt\tFirstInt\n"
                                    + KEY_COUNT + "\t" + KEY_COUNT + "\t" + KEY_COUNT + "\t" + KEY_COUNT + "\n");
                }, configuration, LOG);
            }
        });
    }
}
