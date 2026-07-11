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
import io.questdb.std.ObjList;
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
 * The table gives every key exactly one non-null value per type column and many NULLs. For
 * last_not_null the non-null sits at the key's first (lowest) rowId, so an all-null slot built from
 * later rows carries a higher rowId; for first_not_null the non-null sits at the key's last
 * (highest) rowId, so an all-null slot built from earlier rows carries a lower rowId. Either way the
 * all-null slot sorts ahead of the real value and a guardless merge drops it. The correct answer is
 * non-null for all KEY_COUNT keys, so the count of keys whose aggregate is non-null must equal
 * KEY_COUNT, for every type column.
 * <p>
 * One table carries one value column per type, and one query aggregates all of them at once, so a
 * single grouped scan covers every type: the key hashing, frame dispatch and shard merge run once
 * per iteration instead of once per type. {@code assertEveryTypeKeepsNonNull} asserts a separate
 * count per type column, and each count is aliased with its type name, so a dropped value still
 * names the type it belongs to. A plan guard keeps the query on the parallel factory, without which
 * no shard merge would run at all.
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
    // One case per value type. valueExpr produces the single non-null value of that type; the label
    // names the type's column, so a failing count identifies the type it belongs to.
    private static final ObjList<TypeCase> TYPES = new ObjList<>();

    static {
        TYPES.add(new TypeCase("Char", "'a'::char"));
        TYPES.add(new TypeCase("Date", "100_000::date"));
        TYPES.add(new TypeCase("Decimal", "1.5::decimal(18,3)"));
        TYPES.add(new TypeCase("Double", "1.5::double"));
        TYPES.add(new TypeCase("Float", "1.5::float"));
        TYPES.add(new TypeCase("GeoHash", "#u"));
        TYPES.add(new TypeCase("IPv4", "ipv4 '10.0.0.1'"));
        TYPES.add(new TypeCase("Int", "42::int"));
        TYPES.add(new TypeCase("Long", "42::long"));
        TYPES.add(new TypeCase("Str", "'abc'"));
        TYPES.add(new TypeCase("Symbol", "'abc'::symbol"));
        TYPES.add(new TypeCase("Timestamp", "100_000::timestamp"));
        TYPES.add(new TypeCase("Uuid", "'00000000-0000-0000-0000-000000000001'::uuid"));
        TYPES.add(new TypeCase("Varchar", "'abc'::varchar"));
    }

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

    // Runs the merge invariant for every value type in a single grouped query. The table holds one
    // value column per type (single non-null value per key, placed where nonNullCondition holds; the
    // CASE has no ELSE, so every other row of the key is NULL of the same type). The query applies
    // the aggregate to all of them and counts, per type, the keys whose aggregate survived. It runs
    // ITERATIONS times, since the buggy merge direction is hit only on some runs.
    private void assertEveryTypeKeepsNonNull(String func, String nonNullCondition) throws Exception {
        final StringBuilder columns = new StringBuilder();
        final StringBuilder aggregates = new StringBuilder();
        final StringBuilder counts = new StringBuilder();
        final StringBuilder expected = new StringBuilder();
        final StringBuilder expectedRow = new StringBuilder();
        for (int i = 0, n = TYPES.size(); i < n; i++) {
            final TypeCase type = TYPES.getQuick(i);
            if (i > 0) {
                columns.append(", ");
                aggregates.append(", ");
                counts.append(", ");
                expected.append('\t');
                expectedRow.append('\t');
            }
            columns.append("CASE WHEN ").append(nonNullCondition).append(" THEN ").append(type.valueExpr)
                    .append(" END AS v").append(type.label);
            aggregates.append(func).append("(v").append(type.label).append(") a").append(type.label);
            // Alias each count with its type, so a dropped value names the type in the failure diff.
            counts.append("count(a").append(type.label).append(") ").append(type.label);
            expected.append(type.label);
            expectedRow.append(KEY_COUNT);
        }
        final String createSql = "CREATE TABLE tab AS (" +
                "  SELECT (x % " + KEY_COUNT + ")::int AS g, " + columns +
                "  FROM long_sequence(" + ROW_COUNT + ")" +
                ")";
        final String query = "SELECT " + counts + " FROM (SELECT g, " + aggregates + " FROM tab)";

        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (ignore, compiler, ctx) -> {
                    execute(compiler, createSql, ctx);
                    // Without the parallel factory there is no shard merge left to guard.
                    TestUtils.printSql(compiler, ctx, "EXPLAIN " + query, sink);
                    TestUtils.assertContains(sink, "Async Group By");
                    // Every key has exactly one non-null value per type, so a correct aggregate is
                    // non-null for all KEY_COUNT keys. A guardless merge drops some on most runs.
                    for (int i = 0; i < ITERATIONS; i++) {
                        assertQuery(query)
                                .noLeakCheck()
                                .withCompiler(compiler)
                                .withContext(ctx)
                                .noRandomAccess()
                                .expectSize()
                                .returns(expected + "\n" + expectedRow + "\n");
                    }
                }, configuration, LOG);
            }
        });
    }

    private static class TypeCase {
        private final String label;
        private final String valueExpr;

        private TypeCase(String label, String valueExpr) {
            this.label = label;
            this.valueExpr = valueExpr;
        }
    }
}
