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
 * End-to-end regression test for the {@code first_not_null} / {@code last_not_null} parallel
 * merge invariant, across every value type.
 * <p>
 * Parallel keyed GROUP BY reduces page frames into per-worker (and owner) maps, then merges shard
 * maps pairwise via {@link io.questdb.griffin.engine.groupby.GroupByMergeShardJob}. The inherited,
 * unconditional {@code computeFirst} stores the first row of a key even when it is NULL, with a
 * real rowId. So a map whose only rows for a key are NULL ends with a slot holding
 * {@code (realRowId, NULL)}. When another map holds the single non-null value for that key at a
 * different rowId, {@code merge} must keep the non-null value. A merge that compares rowId alone
 * (last: {@code srcRowId > destRowId}; first: {@code srcRowId < destRowId}) drops it whenever the
 * all-null slot's rowId sorts the wrong way. Every merge must additionally accept the src when the
 * dest slot still holds a NULL value.
 * <p>
 * Each table gives every key exactly one non-null value and many NULLs. For last_not_null the
 * non-null sits at the key's first (lowest) rowId, so an all-null slot built from later rows
 * carries a higher rowId; for first_not_null the non-null sits at the key's last (highest) rowId,
 * so an all-null slot built from earlier rows carries a lower rowId. Either way the all-null slot
 * sorts ahead of the real value and a guardless merge drops it. The correct answer is non-null for
 * all KEY_COUNT keys, so the count of keys whose aggregate is non-null must equal KEY_COUNT.
 * <p>
 * Scope - this test exercises the merge dest-null guard only. For last_not_null that guard is the
 * defect fixed on this branch (the merge previously compared rowId alone), so the pre-fix code
 * drops values here on essentially every run. For first_not_null the guard already existed, so
 * these cases are a regression guard that it stays in place - deleting the first_not_null dest-null
 * guard makes them fail with the same signature. They do NOT cover the separate computeNext
 * ordering defect (a slot receiving a key's rows out of rowId order). The min-rowId merge corrects
 * a single slot's ordering error unless two non-null rows of one key happen to land in the same
 * slot in reverse order, a scheduling window that cannot be forced from SQL; a count-based,
 * single-non-null fixture like this one therefore cannot give the first_not_null ordering fix
 * teeth. That defect is covered deterministically, per type and per width, by
 * {@link FirstLastParallelOrderingTest}, which drives computeFirst/computeNext directly with
 * descending rowIds.
 * <p>
 * The split of a key's rows across page frames - and the frame-to-worker dispatch - is dynamic, so
 * any single key may or may not hit the buggy merge direction on a given run; with many keys a
 * guardless merge drops at least one value on essentially every run. The query runs repeatedly to
 * make the failure reliable rather than seed-dependent.
 */
public class FirstLastNotNullParallelMergeTest extends AbstractCairoTest {

    private static final int ITERATIONS = 10;
    private static final int KEY_COUNT = 1000;
    private static final int ROW_COUNT = 100_000;
    // last_not_null: non-null at each key's first (lowest-rowId) occurrence.
    private static final String FIRST_OCCURRENCE = "x <= " + KEY_COUNT;
    // first_not_null: non-null at each key's last (highest-rowId) occurrence.
    private static final String LAST_OCCURRENCE = "x > " + (ROW_COUNT - KEY_COUNT);
    // {label, value expression} for every value type. valueExpr produces the single non-null value
    // of that type; the label identifies the type in a failure message. All types run in one shared
    // WorkerPool/engine per function (see assertEveryTypeKeepsNonNull) so the expensive per-test
    // fixture (fresh engine + worker pool + memory-leak scaffolding) is paid twice, not 28 times.
    private static final String[][] TYPES = {
            {"Char", "'a'::char"},
            {"Date", "100000::date"},
            {"Decimal", "1.5::decimal(18,3)"},
            {"Double", "1.5::double"},
            {"Float", "1.5::float"},
            {"GeoHash", "#u"},
            {"IPv4", "ipv4 '10.0.0.1'"},
            {"Int", "42::int"},
            {"Long", "42::long"},
            {"Str", "'abc'"},
            {"Symbol", "'abc'::symbol"},
            {"Timestamp", "100000::timestamp"},
            {"Uuid", "'00000000-0000-0000-0000-000000000001'::uuid"},
            {"Varchar", "'abc'::varchar"},
    };

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        // Force sharding and small frames so a key's rows scatter across many worker maps.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 64);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 2);
        super.setUp();
    }

    @Test
    public void testFirstNotNullAllTypes() throws Exception {
        assertEveryTypeKeepsNonNull("first_not_null", LAST_OCCURRENCE);
    }

    @Test
    public void testLastNotNullAllTypes() throws Exception {
        assertEveryTypeKeepsNonNull("last_not_null", FIRST_OCCURRENCE);
    }

    // Runs the merge invariant for every value type against a single shared engine and worker pool.
    // Each type builds its own table (single non-null value per key, placed where nonNullCondition
    // holds; the CASE has no ELSE, so every other row of the key is NULL of the same type) and runs
    // the aggregate ITERATIONS times, since the buggy merge direction is hit only on some runs.
    private void assertEveryTypeKeepsNonNull(String func, String nonNullCondition) throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (ignore, compiler, ctx) -> {
                    for (String[] type : TYPES) {
                        final String label = type[0];
                        final String valueExpr = type[1];
                        final String createSql = "CREATE TABLE tab AS (" +
                                "  SELECT (x % " + KEY_COUNT + ")::int AS g," +
                                "         CASE WHEN " + nonNullCondition + " THEN " + valueExpr + " END AS v" +
                                "  FROM long_sequence(" + ROW_COUNT + ")" +
                                ")";
                        final String query = "SELECT count(*) FROM (SELECT g, " + func + "(v) lv FROM tab) WHERE lv IS NOT NULL";
                        try {
                            execute(compiler, "DROP TABLE IF EXISTS tab", ctx);
                            execute(compiler, createSql, ctx);
                            // Every key has exactly one non-null value, so a correct aggregate is
                            // non-null for all KEY_COUNT keys. A guardless merge drops some on most runs.
                            for (int i = 0; i < ITERATIONS; i++) {
                                assertQuery(query)
                                        .noLeakCheck()
                                        .withCompiler(compiler)
                                        .withContext(ctx)
                                        .noRandomAccess()
                                        .expectSize()
                                        .returns("count\n" + KEY_COUNT + "\n");
                            }
                        } catch (AssertionError e) {
                            throw new AssertionError("type=" + label + " func=" + func + ": " + e.getMessage(), e);
                        }
                    }
                }, configuration, LOG);
            }
        });
    }
}
